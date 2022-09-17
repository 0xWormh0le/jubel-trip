import PropTypes from 'prop-types';
import React from 'react';
import Rating from 'react-rating';

import Icon from './Icon';

const RatingControl = ({
  size,
  emptyIcon,
  fullIcon,
  color,
  fullColor,
  ...props
}) => (
  <Rating
    {...props}
    emptySymbol={<Icon size={size} name={emptyIcon} color={color} />}
    fullSymbol={<Icon size={size} name={fullIcon} color={fullColor} />}
  />
);

RatingControl.displayName = 'RatingControl';

RatingControl.propTypes = {
  size: PropTypes.number,
  emptyIcon: PropTypes.string,
  fullIcon: PropTypes.string,
  color: PropTypes.string,
  fullColor: PropTypes.string,
  ...Rating.propTypes,
};

RatingControl.defaultProps = {
  size: 28,
  emptyIcon: 'star-border',
  fullIcon: 'star',
  color: 'border',
  fullColor: 'primary',
  ...Rating.defaultProps,
};

export default RatingControl;
