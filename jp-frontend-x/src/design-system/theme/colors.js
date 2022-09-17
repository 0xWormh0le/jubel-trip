import { camelCase } from 'lodash';
import tinycolor from 'tinycolor2';

// non-mutating versions of tinycolor methods
export const TinyColor = (color) => {
  if (color instanceof tinycolor) {
    return color.clone();
  }
  return tinycolor(color);
};
export const darker = (color, amount = 10) => TinyColor(color).darken(amount);
export const lighter = (color, amount = 10) => TinyColor(color).lighten(amount);
export const alpha = (color, amount = 1) => TinyColor(color).setAlpha(amount);

// https://www.color-blindness.com/color-name-hue/
export const palette = [
  ['allports', '#00709c'],
  ['aluminium', '#aaaebc'],
  ['athens gray', '#f5f6f8'],
  ['black', '#000000'],
  ['buff', '#f1de85'],
  ['carnation red', '#f66161'],
  ['cornflower blue', '#69a5f1'],
  ['de york', '#79cd7f'],
  ['dove gray', '#636363'],
  ['ebony clay', '#252935'],
  ['french gray', '#bcbfca'],
  ['gunsmoke', '#8a8d8d'],
  ['hawkes blue', '#d4e2fc'],
  ['light green', '#a2f48f'],
  ['link water', '#d9dbe3'],
  ['lochmara', '#008bc2'],
  ['malibu', '#6a8ffe'],
  ['cerulean', '#00b4fa'],
  ['manatee', '#878c9e'],
  ['mauve', '#cd9ff8'],
  ['milano red', '#cc2200'],
  ['mischka', '#dddfe8'],
  ['nobel', '#b7b7b7'],
  ['river bed', '#40475c'],
  ['rob roy', '#efcb69'],
  ['santas gray', '#9d9ea4'],
  ['shamrock', '#49df85'],
  ['stratos', '#000e45'],
  ['tundora', '#464646'],
  ['turquoise', '#64e4a9'],
  ['waterloo', '#7c7e8b'],
  ['white', '#ffffff'],
  ['anakiwa', '#acd8fe'],
  ['golden sand', '#f0cc69'],
  ['snow', '#fcfcfc'],
  ['oc', '#69a6f1'],
  ['an', '#64e5aa'],
  ['cn', '#a3f490'],
  ['cu', '#cea0f8'],
  ['pp', '#f2690d'],
  ['react select blue', '#deebff'],
].reduce((p, [name, hex]) => ({ ...p, [camelCase(name)]: tinycolor(hex) }), {});

const {
  an,
  athensGray,
  black,
  carnationRed,
  cerulean,
  cn,
  cornflowerBlue,
  cu,
  deYork,
  ebonyClay,
  frenchGray,
  goldenSand,
  malibu,
  manatee,
  // milanoRed,
  mischka,
  oc,
  pp,
  reactSelectBlue,
  robRoy,
  snow,
  tundora,
  white,
} = palette;

const travelStyleColors = (c, t = white, d = athensGray) => ({
  bg: c.toString(),
  text: t.toString(),
  border: darker(c, 15).toString(),
  disabled: {
    bg: alpha(c, 0.3).toString(),
    text: d.toString(),
    border: alpha(c, 0.25).toString(),
  },
});

export const colors = {
  // backgrounds
  primary: cerulean.toString(),
  secondary: deYork.toString(),
  info: cornflowerBlue.toString(),
  success: deYork.toString(),
  active: reactSelectBlue.toString(),
  warning: robRoy.toString(),
  error: carnationRed.toString(),
  danger: carnationRed.toString(),
  background: athensGray.toString(),
  disabled: 'transparent',
  surface: white.toString(),
  border: frenchGray.toString(),
  lightBg: snow.toString(),

  // foregrounds
  on: {
    primary: white.toString(),
    secondary: white.toString(),
    info: white.toString(),
    success: white.toString(),
    active: ebonyClay.toString(),
    warning: ebonyClay.toString(),
    error: white.toString(),
    danger: white.toString(),
    background: tundora.toString(),
    surface: ebonyClay.toString(),
    border: white.toString(),
    disabled: manatee.toString(),
  },

  // alternative colors go here that don't fit in the above scheme
  // should have functionally named keys, not color names
  misc: {
    muted: manatee.toString(),
    search: {
      bg: alpha(white, 0.7).toString(),
    },
    overlay: {
      bg: alpha(black, 0.4).toString(),
      color: white.toString(),
    },
    map: {
      overlay: 'rgba(109,187,255,.3)',
      marker: {
        normal: ebonyClay.toString(),
        satellite: white.toString(),
      },
      route: {
        normal: ebonyClay.toString(),
        satellite: white.toString(),
      },
      on: {
        route: {
          normal: white.toString(),
          satellite: ebonyClay.toString(),
        },
      },
    },
    outline: alpha('deepskyblue', 0.5).toString(),
    filter: {
      text: manatee.toString(),
    },
    story: mischka.toString(),
    devOnly: {
      bg: robRoy.toString(),
    },
    slider: {
      rail: mischka.toString(),
      track: lighter(manatee).toString(),
      handle: manatee.toString(),
      text: manatee.toString(),
      shadow: 6,
    },
    datePicker: {
      today: {
        bg: athensGray.toString(),
        color: darker(mischka).toString(),
      },
      rangeEndDays: {
        bg: malibu.toString(),
        color: white.toString(),
      },
      rangeInsideDays: {
        bg: lighter(malibu).toString(),
        color: white.toString(),
      },
      rangeOutsideDays: {
        bg: athensGray.toString(),
        color: tundora.toString(),
      },
    },
    reviews: {
      fullColor: goldenSand.toString(),
    },
    branding: {
      styles: {
        oc: travelStyleColors(oc),
        an: travelStyleColors(an),
        cn: travelStyleColors(cn),
        cu: travelStyleColors(cu),
        pp: travelStyleColors(pp),
      },
    },
  },

  selects: {
    primaryColor: malibu.toString(),
    optionFocusBg: alpha(malibu, 0.07).toString(),
    tagBorder: alpha(frenchGray, 0.4).toString(),
    menuShadow: alpha(black, 0.15).toString(),
  },
};

export const shades = (color) => {
  const base = tinycolor(color);
  const lighter = [52, 37, 26, 12, 6].map((f) => lighter(base, f));
  const darker = [6, 12, 18, 24].map((f) => darker(base, f));
  const alt = [[50, 30], [30, 30], [10, 15], [5, 5]].map(([f, s]) =>
    lighter(base, f).saturate(s)
  );
  return [...lighter, base, ...darker, ...alt];
};

export const hover = (color) => {
  const base = tinycolor(color);
  return base.isLight() ? darker(base, 6) : lighter(base, 6);
};

export const greyOut = (color) => {
  const base = tinycolor(color);
  return lighter(base, 8);
};

export const rgbaArray = (color) => {
  const { r, g, b, a } = tinycolor(color).toRgb();
  return [r, g, b, Math.round(a * 255)];
};

export default colors;
