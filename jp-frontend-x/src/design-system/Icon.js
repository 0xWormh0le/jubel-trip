import cleanElement from 'clean-element';
import React from 'react';

import Flex from './Flex';
import system from '../utils/System';
import theme from './theme';

const { icons } = theme;

// Remove `space` props from the `svg` element prevents react warnings
const CleanSvg = cleanElement('svg');

const Base = (props) => {
  const { name, size, ...otherProps } = props;
  const icon = icons[name];
  if (!icon) return <Flex width={size} height={size} />;

  return (
    <Flex centered {...otherProps}>
      {typeof icon === 'function' ? (
        icon(props)
      ) : (
        <CleanSvg
          viewBox={icon.viewBox}
          width={size}
          height={size}
          fill="currentcolor"
        >
          <path d={icon.body[0].props.d} />
        </CleanSvg>
      )}
    </Flex>
  );
};

const Icon = system(
  'Icon',
  {
    extend: Base,
    name: 'check',
    size: 24,
  },
  ({ size, spin }) => ({
    minWidth: size,
    animation: spin && 'spin 2s linear infinite',
  })
);

export default Icon;
