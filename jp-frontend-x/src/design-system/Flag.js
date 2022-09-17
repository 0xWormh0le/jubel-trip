import styled from '@emotion/styled';
import React from 'react';
import { borderRadius, themeGet } from 'styled-system';

import system from '../utils/System';
import Box from './Box';
import Flex from './Flex';
import Hide from './Hide';

const capitalize = (str) => str.charAt(0).toUpperCase() + str.slice(1);

const flexAuto = (props) =>
  props.flexAuto
    ? {
        flex: '1 1 auto',
      }
    : null;

const shadowColor = (props) => {
  const darkColor = themeGet(`colors.${capitalize(props.color)}`)(props);

  return {
    backgroundImage: !darkColor
      ? `
        linear-gradient(45deg, transparent 50%, rgba(0, 0, 0, 0.5) 50%),
        linear-gradient(45deg, transparent 50%, ${props.color} 50%)
      `
      : `linear-gradient(45deg, transparent 50%, ${darkColor} 50%)`,
  };
};
const FlagShadow = system(
  {
    extend: Box,
    bottom: 0,
    color: 'background',
  },
  shadowColor,
  (props) => ({
    position: 'absolute',
    alignSelf: 'flex-end',
  })
);

const FlagRight = system(
  {
    extend: Box,
    borderRadius: 'sm',
    color: 'primary',
  },
  (props) => ({
    flex: 'none',
    transform: 'skew(-20deg)',
    position: 'relative',
    zIndex: 1,
    bg: themeGet(`colors.${props.color}`, props.color)(props),
    borderRadius: `0 ${themeGet(`radii.${props.borderRadius}`)} ${themeGet(
      `radii.${props.borderRadius}`
    )} 0`,
  })
);

const FlagBody = system(
  {
    extend: Box,
    fontSize: 0,
    borderRadius: 'sm',
    bg: 'primary',
    color: 'background',
  },
  borderRadius,
  flexAuto,
  (props) => ({
    borderRadius: `0 0 ${themeGet(`radii.${props.borderRadius}`)} 0`,
    zIndex: 2,
  })
);

const RelativeHide = styled(Hide)`
  position: relative;
`;

const Flag = ({ color, bg, children, width, ...props }) => (
  <Flex width={width} {...props} ml={[0, -2]}>
    <RelativeHide xs>
      <FlagShadow width="4px" mr={-2} mb={-2} color={bg} />
    </RelativeHide>
    <FlagBody
      flexAuto={!!width}
      color={color}
      background={bg}
      py={[1, 2]}
      pl={[1, 3]}
    >
      {children}
    </FlagBody>
    <FlagRight width="18px" color={bg} ml={-2} />
  </Flex>
);

Flag.displayName = 'Flag';

export default Flag;
