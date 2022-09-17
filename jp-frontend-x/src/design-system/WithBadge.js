import PropTypes from 'prop-types';
import React from 'react';

import Absolute from './Absolute';
import Badge from './Badge';
import Relative from './Relative';

const WithBadge = ({
  children,
  label,
  position = { top: '.5em', left: 0 },
  bg,
  color,
  ...props
}) => (
  <Relative {...props}>
    {children}
    <Absolute {...position}>
      <Badge error>{label}</Badge>
    </Absolute>
  </Relative>
);

WithBadge.displayName = 'WithBadge';

WithBadge.propTypes = {
  content: PropTypes.string || PropTypes.number,
  position: PropTypes.string,
  bg: PropTypes.string,
  color: PropTypes.string,
};

export default WithBadge;
