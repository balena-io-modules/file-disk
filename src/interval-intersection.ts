export type Interval = [number, number];

export function intervalIntersection(
	a: Interval,
	b: Interval,
): Interval | null {
	const start = Math.max(a[0], b[0]);
	const end = Math.min(a[1], b[1]);
	if (start > end) {
		return null;
	}
	return [start, end];
}
