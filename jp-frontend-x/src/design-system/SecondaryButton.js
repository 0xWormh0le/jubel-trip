import { themeGet } from 'styled-system';

import system from '../utils/System';
import Button from './Button';

const SecondaryButton = system(
  'SecondaryButton',
  {
    extend: Button,
    bg: 'secondary',
  },
  (props) => ({
    '&:hover': {
      background: props.disabled
        ? null
        : themeGet(`colors.${props.hoverBg}`)(props),
    },
  })
);

export default SecondaryButton;
