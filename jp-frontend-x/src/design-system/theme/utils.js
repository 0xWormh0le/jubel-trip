// some utility functions
export const unit = (v, u) => (Number.isFinite(v) ? `${v}${u}` : v);
export const rem = (v) => unit(v, 'rem');
export const em = (v) => unit(v, 'em');
export const px = (v) => unit(v, 'px');
export const pct = (v) => unit(v, '%');

export const round = (value) => Math.round(value * 1e3) / 1e3;

export const rem2px = (s, baseFontSize = 16) =>
  round(parseFloat(s.replace(/rem$/, '')) * baseFontSize);

export const mapValues = (o, f) =>
  Object.keys(o).reduce((obj, k) => {
    obj[k] = f(o[k]); // eslint-disable-line no-param-reassign
    return obj;
  }, {});

// scale generator
const intervals = {
  m2: 1.067, // minor second
  M2: 1.125, // major second
  m3: 1.2, // minor third
  M3: 1.25, // major third
  P4: 1.333, // perfect fourth
  A4: 1.414, // augmented fourth
  P5: 1.5, // perfect fifth
  GR: 1.618, // golden ratio
};
// iv - the interval;
// half - number of elements to generate above and below unit;
export const genInterval = ({ iv = 'M3', half = 5, addZero = false }) => {
  const f = intervals[iv];
  const scale = [];
  if (addZero) scale.push(0);
  for (let i = -half; i <= half; i += 1) {
    scale.push(round(f ** i));
  }
  return scale;
};

export const flagEmoji = (code) =>
  String.fromCodePoint(
    ...code
      .toUpperCase()
      .split('')
      .map((c) => c.codePointAt(0) + 0x1f1a5)
  );
