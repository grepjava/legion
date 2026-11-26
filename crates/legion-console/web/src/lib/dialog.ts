// A modal <dialog closedby="any"> closes on a click outside it. Browsers
// without `closedby` (Safari, older Firefox) need this as its onclick:
// a click on the backdrop lands on the dialog itself, outside its box.

/** Closes `dialog` when `e`, a click on it, was outside its box. */
export function lightDismiss(e: MouseEvent, dialog: HTMLDialogElement | undefined) {
  if (!dialog || 'closedBy' in HTMLDialogElement.prototype) return;
  if (e.target !== dialog) return;
  const r = dialog.getBoundingClientRect();
  const inside = r.top <= e.clientY && e.clientY <= r.bottom && r.left <= e.clientX && e.clientX <= r.right;
  if (!inside) dialog.close();
}
