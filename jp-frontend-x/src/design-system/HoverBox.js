import PropTypes from 'prop-types';

import dynamicVariant from '../utils/DynamicVariant';
import system from '../utils/System';
import Box from './Box';

const hoverVariant = dynamicVariant({
  key: 'hovers',
  prop: 'hover',
});

const HoverBox = system(
  'HoverBox',
  {
    extend: Box,
    bg: 'surface',
    color: 'on.surface',
    hover: 'bg.darken',
  },
  hoverVariant
);

HoverBox.propTypes = {
  hover: PropTypes.string,
};

export default HoverBox;
