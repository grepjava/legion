<script lang="ts">
  // An icon at the console's weight: hidden from assistive technology unless
  // it is all there is to say (a label), in the colour of what holds it
  // (muted by default, see .ico in app.css).
  import type { IconComponent } from './icons';

  let {
    icon: I,
    size = 16,
    label = '',
    fill = false,
    class: cls = ''
  }: { icon: IconComponent; size?: number; label?: string; fill?: boolean; class?: string } =
    $props();
</script>

<I
  {size}
  strokeWidth={1.75}
  absoluteStrokeWidth={size < 16}
  fill={fill ? 'currentColor' : 'none'}
  class="ico {cls}"
  aria-hidden={label ? undefined : 'true'}
  aria-label={label || undefined}
  role={label ? 'img' : undefined}
  focusable="false"
/>
