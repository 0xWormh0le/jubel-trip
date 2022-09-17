import get from 'lodash/get';

import system from '../utils/System';
import Box from './Box';

export const Hr = system(
  'Hr',
  {
    extend: Box,
    as: 'hr',
    color: 'border',
    my: 3,
  },
  ({ color, my, theme }) => ({
    border: 'none',
    borderBottom: `${theme.borders[2]} ${get(theme, `colors.${color}`)}`,
    margin: `${get(theme, `space.${my}`)} 0`,
  })
);

export default Hr;
