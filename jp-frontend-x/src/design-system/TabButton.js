import React from 'react';

import targetedProps from '../utils/targetedProps';
import Button from './Button';

const defaults = {
  borderRadius: 'top.sm',
  borderColor: 'transparent',
  borderTop: 0,
  borderBottom: 4,
  borderLeft: 0,
  borderRight: 0,
  boxShadow: 0,
  bold: false,
  uppercase: true,
  bg: 'surface',
  px: 3,
  py: 3,
  minWidth: 'auto',
  color: 'misc.muted',
  letterSpacing: 'tracked',
};

const activeDefaults = {
  borderColor: 'secondary',
  bold: true,
  color: 'on.background',
};

const TabButton = ({ active = false, children, ...props }) => {
  const activeProps = { ...activeDefaults, ...targetedProps(props, 'active') };
  return (
    <Button {...defaults} {...(active && activeProps)} {...props}>
      {children}
    </Button>
  );
};

export default TabButton;
