import { themeGet } from 'styled-system';
import { hover, greyOut } from '../design-system/theme/colors';

const hoverColors = (props) => {
  const {
    disabled,
    active,
    primary,
    secondary,
    success,
    warning,
    info,
    error,
    danger,
    color,
    bg,
    borderColor,
    _bg, // if true, we want the background color
    _hover, // if true, we're in hover state
    _prop = 'bg', // the property to
  } = props;

  let key = null;
  let result = null;

  // asking for border?
  const _border = _prop === 'borderColor';

  // asking for on (fg/text) color?
  const _on = !_bg && !_border;
  let specified = false;

  if (active) key = 'active';
  else if (primary) key = 'primary';
  else if (secondary) key = 'secondary';
  else if (success) key = 'success';
  else if (warning) key = 'warning';
  else if (info) key = 'info';
  else if (error) key = 'error';
  else if (danger) key = 'danger';
  else if (_bg && bg) {
    specified = true;
    key = bg;
  } else if (_on && color) {
    specified = true;
    key = color;
  } else if (_border) {
    key = 'border';
    if (borderColor) {
      key = borderColor;
      specified = true;
    }
  }

  if (key) {
    if (_on && !specified) key = `on.${key}`;
    result = themeGet(`colors.${key}`)(props);
    if (result) {
      if (disabled) {
        const g = greyOut(result);
        if (g) result = g.toString();
      } else if (_hover) {
        const h = hover(result);
        if (h) result = h.toString();
      }
    }
  }
  return result;
};

export default hoverColors;
