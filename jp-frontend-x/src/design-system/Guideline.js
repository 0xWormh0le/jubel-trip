import React from 'react';

import Absolute from './Absolute';
import ScreenOverlay from './ScreenOverlay';
import Text from './Text';

const Guideline = ({ zIndex = 101, left = '50%', borderStyle = 'dashed' }) => (
  <ScreenOverlay zIndex={zIndex} bg="transparent">
    <Absolute
      width="1px"
      height="100%"
      borderLeft={1}
      borderColor="white"
      left={left}
      borderStyle={borderStyle}
    >
      <Text
        info
        p={2}
        fontSize={4}
        position="absolute"
        top="50%"
        transform="rotate(-90deg)"
      >
        {left}px
      </Text>
    </Absolute>
  </ScreenOverlay>
);

export default Guideline;
