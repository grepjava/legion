// "Deploy from an image" (CONSOLE.md phase 5): a form's answers as the
// objects `legion apply` takes, a Service and, when asked, a new Volume
// for it. The form writes YAML only; the server checks it on apply.

export type Probe = 'none' | 'http' | 'tcp';

export interface DeployForm {
  name: string;
  namespace: string;
  image: string;
  replicas: number;
  /** Arguments replacing the image's command, one per line; none keeps it. */
  command: string;
  /** The port the container listens on; 0 for none. */
  port: number;
  /** Reachable on every node at this port, through the routing mesh; 0 to
   * stay inside the cluster. */
  publishedPort: number;
  /** `NAME=value`, one per line. */
  env: string;
  /** Where a Volume is mounted; empty for none. */
  mountPath: string;
  /** The Volume's name: one there is, or one made here with `volumeSize`. */
  volume: string;
  /** A size such as `1Gi` makes the Volume; empty uses one there is. */
  volumeSize: string;
  probe: Probe;
  /** For an HTTP probe. */
  probePath: string;
  /** Such as `256Mi`; empty for no limit. */
  memory: string;
  /** Such as `500m`; empty for no request. */
  cpu: string;
}

export function blank(namespace: string): DeployForm {
  return {
    name: '',
    namespace,
    image: '',
    replicas: 1,
    command: '',
    port: 0,
    publishedPort: 0,
    env: '',
    mountPath: '',
    volume: '',
    volumeSize: '',
    probe: 'none',
    probePath: '/',
    memory: '',
    cpu: ''
  };
}

/** The env lines as `{name, value}`, or why a line is wrong. */
export function envVars(text: string): { name: string; value: string }[] | string {
  const out: { name: string; value: string }[] = [];
  for (const [i, raw] of text.split('\n').entries()) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) return `environment line ${i + 1}: write NAME=value`;
    out.push({ name: line.slice(0, eq).trim(), value: line.slice(eq + 1) });
  }
  return out;
}

/** The objects the form describes, in the order to apply them, or why it
 * cannot describe any yet. */
export function objects(f: DeployForm): object[] | string {
  const name = f.name.trim();
  const image = f.image.trim();
  if (!name) return 'give the service a name';
  if (!f.namespace) return 'choose a namespace';
  if (!image) return 'give an image';
  const env = envVars(f.env);
  if (typeof env === 'string') return env;
  const mount = f.mountPath.trim();
  const volume = f.volume.trim() || name;
  if (f.probe !== 'none' && !f.port) return 'a probe needs the port the container listens on';
  if (f.publishedPort && !f.port) return 'publishing needs the port the container listens on';

  const container: Record<string, unknown> = { name, image };
  const command = f.command
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);
  if (command.length) container.command = command;
  if (env.length) container.env = env;
  if (f.port) container.ports = [{ name: 'main', containerPort: f.port }];
  const requests: Record<string, string> = {};
  const limits: Record<string, string> = {};
  if (f.cpu.trim()) requests.cpu = f.cpu.trim();
  if (f.memory.trim()) {
    requests.memory = f.memory.trim();
    limits.memory = f.memory.trim();
  }
  const resources: Record<string, unknown> = {};
  if (Object.keys(requests).length) resources.requests = requests;
  if (Object.keys(limits).length) resources.limits = limits;
  if (Object.keys(resources).length) container.resources = resources;
  if (f.probe === 'http') {
    container.probes = { readiness: { http: { path: f.probePath.trim() || '/', port: 'main' } } };
  } else if (f.probe === 'tcp') {
    container.probes = { readiness: { tcp: { port: 'main' } } };
  }
  if (mount) container.volumeMounts = [{ name: 'data', path: mount }];

  const pod: Record<string, unknown> = { containers: [container] };
  if (mount) pod.volumes = [{ name: 'data', volume: { name: volume } }];
  const spec: Record<string, unknown> = { mode: 'replicated', replicas: f.replicas, pod };
  if (f.port) {
    const port: Record<string, unknown> = { name: 'main', port: f.port, targetPort: 'main' };
    if (f.publishedPort) {
      port.publish = 'ingress';
      port.publishedPort = f.publishedPort;
    }
    spec.endpoint = { mode: 'vip', ports: [port] };
  }

  const out: object[] = [];
  if (mount && f.volumeSize.trim()) {
    out.push({
      apiVersion: 'legion/v1',
      kind: 'Volume',
      metadata: { name: volume, namespace: f.namespace },
      spec: { size: f.volumeSize.trim() }
    });
  }
  out.push({
    apiVersion: 'legion/v1',
    kind: 'Service',
    metadata: { name, namespace: f.namespace },
    spec
  });
  return out;
}
