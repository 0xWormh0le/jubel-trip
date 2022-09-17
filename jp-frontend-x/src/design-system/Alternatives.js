import React from 'react';
import { themeGet } from 'styled-system';

import system from '../utils/System';
import Absolute from './Absolute';
import Box from './Box';
import Flex from './Flex';
import Icon from './Icon';
import Text from './Text';

const AlternativesContainer = system(
  'AlternativesContainer',
  {
    extend: Flex,
  },
  (props) => ({
    position: 'relative',
    '&:hover .alternatives__handle': {
      display: 'flex',
    },
  })
);

const DragIconContainer = system(
  'DragIconContainer',
  {
    extend: Absolute,
  },
  (props) => ({
    top: 0,
    height: `${props.size}px`,
    width: 20,
    display: 'none',
    background: themeGet(`colors.${props.bg}`)(props),
    opacity: 0.6,
  })
);

const ImageBox = system(
  'ImageBox',
  {
    extend: Flex,
  },
  (props) => ({
    backgroundImage: `url(${props.url})`,
    width: `${props.size}px`,
    height: `${props.size}px`,
    backgroundSize: 'cover',
    alignItems: 'center',
    justifyContent: 'center',
  })
);

const Alternatives = ({ name, imageUrl, size = 270 }) => {
  return (
    <AlternativesContainer>
      <DragIconContainer
        bg="surface"
        size={size}
        className="alternatives__handle"
      >
        <Icon name="drag-vertical" size={20} color="black" />
      </DragIconContainer>
      <Flex>
        <ImageBox p={1} url={imageUrl} mb={4} size={size}>
          <Box px={2}>
            <Text fontSize={5} fontWeight="bold" color="white">
              {name}
            </Text>
          </Box>
        </ImageBox>
      </Flex>
    </AlternativesContainer>
  );
};

Alternatives.displayName = 'Alternatives';

export default Alternatives;
