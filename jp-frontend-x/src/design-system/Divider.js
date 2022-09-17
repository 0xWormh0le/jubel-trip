import React from 'react';

import Absolute from './Absolute';
import Flex from './Flex';
import Text from './Text';

const Divider = ({
  text,
  children,
  thickness = 1,
  length = 1,
  vertical = false,
  p = 2,
  width = 1,
  height = '100%',
  ...props
}) => (
  <Flex
    display="inline-flex"
    position="relative"
    centered
    p={p}
    width={!vertical && width}
    height={vertical && height}
    {...props}
  >
    <Absolute
      width={!vertical && 1}
      height={vertical && '100%'}
      top={!vertical && '50%'}
      left={vertical && '50%'}
      borderTop={!vertical && thickness}
      borderLeft={vertical && thickness}
    >
      &nbsp;
    </Absolute>
    {text && !children && (
      <Text zIndex={1} p={2}>
        {text}
      </Text>
    )}
    {children}
  </Flex>
);

export default Divider;
