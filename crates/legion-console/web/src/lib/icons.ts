// The console's icons: Lucide's line set (ISC), drawn at the console's own
// weight and colours by Ico.svelte. One per kind and per page, so a kind
// looks the same in the side bar, a list, its page's strip, the palette
// and the workbench.
import type { Component } from 'svelte';
import {
  BadgeCheck,
  Bell,
  Box,
  Boxes,
  Cable,
  CalendarClock,
  ChartPie,
  ExternalLink,
  FileCode,
  FileCog,
  FileInput,
  FileText,
  Folder,
  Globe,
  Hammer,
  HardDrive,
  KeyRound,
  Layers,
  LayoutDashboard,
  LayoutTemplate,
  Monitor,
  MonitorCog,
  Moon,
  Network,
  Pin,
  Puzzle,
  Rocket,
  Route,
  ScrollText,
  Server,
  Settings,
  ShieldCheck,
  SquareStack,
  SquareTerminal,
  Stethoscope,
  Sun,
  UserRoundCheck,
  Workflow,
  Wrench,
  Zap
} from '@lucide/svelte';

// Lucide's components take its own props; the console passes only size,
// stroke, class and ARIA, which every one takes.
export type IconComponent = Component<any>;

const kinds: Record<string, IconComponent> = {
  Namespace: Folder,
  Node: Server,
  Service: Boxes,
  Job: Workflow,
  CronJob: CalendarClock,
  Task: Box,
  Secret: KeyRound,
  Config: FileCog,
  Volume: HardDrive,
  NetworkPolicy: ShieldCheck,
  Route: Route,
  RoleBinding: UserRoundCheck,
  ImagePolicy: BadgeCheck,
  Build: Hammer
};

/** A kind's icon; a task running a VM shows a monitor, as its circle does. */
export function kindIcon(kind: string, vm = false): IconComponent {
  if (kind === 'Task' && vm) return Monitor;
  return kinds[kind] ?? Box;
}

const pages: Record<string, IconComponent> = {
  '/': LayoutDashboard,
  '/alerts': Bell,
  '/topology': Network,
  '/network': Cable,
  '/disk': ChartPie,
  '/diagnostics': Stethoscope,
  '/volumes': HardDrive,
  '/images': Layers,
  '/build': Hammer,
  '/stacks': SquareStack,
  '/deploy': Rocket,
  '/templates': LayoutTemplate,
  '/import': FileInput,
  '/apply': FileCode,
  '/cluster': Wrench,
  '/audit': ScrollText,
  '/settings': Settings,
  '/clusters': Globe,
  '/plugins': Puzzle
};

/** A workbench tab's icon, by what it shows. */
export function toolIcon(kind: 'logs' | 'terminal' | 'build' | 'events'): IconComponent {
  return { logs: FileText, terminal: SquareTerminal, build: Hammer, events: Bell }[kind];
}

/** An action's icon in the palette; one that changes the theme shows it. */
export function actionIcon(id: string): IconComponent {
  if (id.startsWith('logs:')) return FileText;
  if (id.startsWith('term:')) return SquareTerminal;
  if (id === 'theme:dark') return Moon;
  if (id === 'theme:light') return Sun;
  if (id === 'theme:system') return MonitorCog;
  return Zap;
}

/** A page's icon, by its path: a kind's list or object takes the kind's. */
export function pageIcon(href: string): IconComponent {
  const kind = /^\/k\/([^/?#]+)/.exec(href)?.[1];
  if (kind) return kindIcon(kind);
  const path = href.split(/[?#]/)[0];
  if (path.startsWith('/plugins/')) return Puzzle;
  return pages[path] ?? (href.startsWith('http') ? ExternalLink : Pin);
}
