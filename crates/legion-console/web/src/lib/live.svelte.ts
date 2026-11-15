// Whether the console's live views are current: each followed stream says
// when it breaks and when it is back, and the layout says so while any is
// broken, so nobody reads a list that stopped moving as the cluster now.
export const live = $state({ down: {} as Record<number, string> });
