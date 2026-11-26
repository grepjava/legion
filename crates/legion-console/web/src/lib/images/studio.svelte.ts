// What the Images page's tabs and dialogs share: the namespace worked in,
// its registry login Secrets, the nodes, and one place to say what
// happened (or went wrong) while something is busy.
import type { LObject, RegistryImageInspect, RegistrySearchHit } from '../api.ts';

/** Starting points for a Containerfile. */
export const TEMPLATES: Record<'minimal' | 'pkg' | 'daemon', string> = {
  minimal:
    'FROM ghcr.io/freebsd/freebsd-runtime:15.1\nCOPY --chmod=0755 <<EOF /usr/local/bin/entrypoint.sh\n#!/bin/sh\necho "FreeBSD jail ready"\nsleep 3600\nEOF\nCMD ["/usr/local/bin/entrypoint.sh"]\n',
  pkg: 'FROM ghcr.io/freebsd/freebsd-notoolchain:15.1\nRUN env ASSUME_ALWAYS_YES=yes pkg install -y nginx ca_root_nss && pkg clean -ay\nEXPOSE 80\nCMD ["/usr/local/sbin/nginx", "-g", "daemon off;"]\n',
  daemon:
    'FROM ghcr.io/freebsd/freebsd-runtime:15.1\nWORKDIR /app\nENV APP_ENV=production\nEXPOSE 8080\nCMD ["/bin/sh", "-c", "echo listening on 8080 && sleep 3600"]\n'
};

/** A Containerfile that installs FreeBSD's package `name`. */
export const fromPackages = (name: string) =>
  `FROM ghcr.io/freebsd/freebsd-notoolchain:15.1\n` +
  `# The package's name as \`pkg search ${name}\` lists it (FreshPorts has them all).\n` +
  `RUN env ASSUME_ALWAYS_YES=yes pkg install -y ${name} && pkg clean -ay\n` +
  `CMD ["/bin/sh"]\n`;

/** A word of a search, as a package name: what pkg(8) would take. */
export const pkgName = (q: string) =>
  q
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._+-]/g, '');

/** The search tab's query, what it found and the image inspected, kept
 * while another tab is open. */
export class SearchDraft {
  query = $state('');
  secret = $state('');
  source: 'all' | 'freebsd' | 'ghcr' | 'docker' = $state('all');
  hits: RegistrySearchHit[] = $state([]);
  /** What the last search was for, once it has answered. */
  searched = $state('');
  inspectInput = $state('');
  inspectSecret = $state('');
  inspected: RegistryImageInspect | null = $state(null);
}

/** The package tab's Build, as far as it is written, kept while another
 * tab is open. */
export class BuildDraft {
  from: 'containerfile' | 'git' = $state('containerfile');
  containerfile = $state(TEMPLATES.minimal);
  gitUrl = $state('');
  gitRef = $state('');
  gitPath = $state('');
  image = $state('ghcr.io/your-org/freebsd-app:v1');
  rollout = $state('');
}

export class Studio {
  readonly search = new SearchDraft();
  readonly build = new BuildDraft();
  error = $state('');
  message = $state('');
  busy = $state(false);
  /** The namespace's Secrets, by name. */
  secrets: string[] = $state([]);
  /** The cluster's nodes, for those who see them. */
  nodes: LObject[] = $state([]);
  // Each form's registry login: a Secret just saved is chosen in every one.
  pullSecret = $state('');
  pushSecret = $state('');
  buildSecret = $state('');

  readonly #namespace: () => string;
  readonly #cannotWrite: () => string | null;

  constructor(namespace: () => string, cannotWrite: () => string | null) {
    this.#namespace = namespace;
    this.#cannotWrite = cannotWrite;
  }

  /** The namespace Secrets, Builds and pushes are in. */
  get namespace() {
    return this.#namespace();
  }

  /** Why the user may not write in it, if they may not. */
  get writeWhy() {
    return this.#cannotWrite();
  }

  /** A Secret just saved: listed, and chosen in every form. */
  saved(name: string) {
    this.secrets = [...new Set([...this.secrets, name])].sort();
    this.pullSecret = this.pushSecret = this.buildSecret = name;
  }

  /** Runs `work` busy, saying what it returns or the error it throws. */
  async act(work: () => Promise<string | void>): Promise<boolean> {
    this.busy = true;
    this.error = '';
    this.message = '';
    try {
      const said = await work();
      if (said) this.message = said;
      return true;
    } catch (e) {
      this.error = (e as Error).message;
      return false;
    } finally {
      this.busy = false;
    }
  }
}
