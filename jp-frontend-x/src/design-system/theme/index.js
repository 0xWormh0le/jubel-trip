import colors, { palette } from './colors';
import {
  fontFamily,
  headingsFamily,
  baseFontSize,
  fontSizes,
  fontWeights,
  htmlFontScale,
} from './fonts';
import shadows from './shadows';
import icons from './icons';
import borders, { borderWidths } from './borders';
import breakpoints, { maxWidths } from './breakpoints';
import radii from './radii';
import letterSpacings from './letterSpacings';
import lineHeights from './lineHeights';
import space from './space';
import textStyles from './textStyles';

export { default as GlobalStyles } from './GlobalStyles';
export * from './utils';

const theme = {
  baseFontSize,
  borders,
  borderWidths,
  breakpoints,
  colors,
  fontFamily,
  fontSizes,
  fontWeights,
  headingsFamily,
  htmlFontScale,
  icons,
  letterSpacings,
  lineHeights,
  maxWidths,
  palette,
  radii,
  shadows,
  space,
  textStyles,
};

export default theme;
