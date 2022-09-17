import { rem } from './utils';

export const borderWidths = [
  0,
  1 / 32,
  1 / 16,
  1 / 8,
  1 / 4,
  1 / 2,
  1,
  2,
  4,
].map(rem);
const borders = borderWidths.map((w) => `${w} solid`);
export default borders;
