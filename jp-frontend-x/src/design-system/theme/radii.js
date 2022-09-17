import { mapValues } from './utils';

// compute for each size
const leftr = (r) => `${r} 0 0 ${r}`;
const topr = (r) => `${r} ${r} 0 0`;
const rightr = (r) => `0 ${r} ${r} 0`;
const bottomr = (r) => `0 0 ${r} ${r}`;

const baseRadii = { xs: '0.125rem', sm: '0.25rem', md: '0.5rem', lg: '1rem' };

const radii = {
  none: 0,
  ...baseRadii,
  // specialized radii
  pill: '9999px',
  circle: '100%',
  // radii for each side (left.sm, right.lg, etc)
  left: mapValues(baseRadii, leftr),
  right: mapValues(baseRadii, rightr),
  top: mapValues(baseRadii, topr),
  bottom: mapValues(baseRadii, bottomr),
};

export default radii;
