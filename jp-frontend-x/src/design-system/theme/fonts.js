import { round, rem, genInterval } from './utils';

export const baseFontSize = 16; // px - 16px is standard
export const htmlFontScale = round(100 * (baseFontSize / 16));
export const fontFamily = "'Open Sans', sans-serif";
export const headingsFamily = "'Fjalla One', sans-serif";
export const fontWeights = [
  'normal', // 400
  'bold', // 700
  100,
  200,
  300,
  400,
  500,
  600,
  700,
  800,
  900,
].reduce((a, w) => {
  a[w] = w; // eslint-disable-line no-param-reassign
  return a;
}, {});

// Major Third font scale with 11 members (5 = 1rem)
// see https://type-scale.com/?size=22&scale=1.250
export const fontSizes = genInterval({ iv: 'M3' }).map(rem);
