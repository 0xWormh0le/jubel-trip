import PropTypes from 'prop-types';

import dynamicVariant from '../utils/DynamicVariant';
import system from '../utils/System';
import Text from './Text';

const hoverVariant = dynamicVariant({
  key: 'hovers',
  prop: 'hover',
});

const HoverText = system(
  'HoverText',
  {
    extend: Text,
    bg: 'surface',
    color: 'on.surface',
    hover: 'links.darkenBg',
  },
  hoverVariant
);

HoverText.propTypes = {
  hover: PropTypes.any,
};

export default HoverText;
