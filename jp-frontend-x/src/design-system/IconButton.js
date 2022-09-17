import isNil from 'lodash/isNil';
import React from 'react';

import Flex from './Flex';
import Icon from './Icon';
import targetedProps from '../utils/targetedProps';
import Text from './Text';

const vert = (lp) => lp === 'top' || lp === 'bottom';
const before = (lp) => lp === 'left' || lp === 'top';

const IconButton = React.forwardRef(
  (
    {
      name,
      size = 24,
      bg = 'transparent',
      color = 'misc.muted',
      disabled = false,
      alignItems = 'center',
      border = 'none',
      label,
      children,
      labelPosition = 'right',
      labelMargin = 1,
      hoverable = true,
      justifyContent,
      labelUppercase = true,
      labelBold = true,
      as = 'button',
      ...props
    },
    ref
  ) => {
    const iconProps = targetedProps(props, 'icon');
    const labelProps = targetedProps(props, 'label');
    const justIcon = isNil(label) && !children;
    return (
      <Flex
        disabled={disabled}
        hoverable={hoverable}
        flexDirection={justIcon || vert(labelPosition) ? 'column' : 'row'}
        flexWrap="nowrap"
        nobr
        justifyContent={justifyContent || (justIcon && 'center')}
        alignItems={alignItems}
        border={border}
        color={color}
        bg={bg}
        as={as}
        ref={ref}
        {...props}
      >
        <Icon
          name={name}
          size={size}
          order={before(labelPosition) ? 2 : 1}
          {...iconProps}
        />
        {(children || !isNil(label)) && (
          <Text
            bg="transparent"
            disabled={disabled}
            order={before(labelPosition) ? 1 : 2}
            ml={labelPosition === 'right' && labelMargin}
            mr={labelPosition === 'left' && labelMargin}
            mt={labelPosition === 'bottom' && labelMargin}
            mb={labelPosition === 'top' && labelMargin}
            truncate
            nobr
            uppercase={labelUppercase}
            bold={labelBold}
            fontSize={3}
            {...labelProps}
          >
            {children || label}
          </Text>
        )}
      </Flex>
    );
  }
);

IconButton.displayName = 'IconButton';

export default IconButton;
