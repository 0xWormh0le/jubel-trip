import styled from '@emotion/styled';
import React from 'react';
import { animated, config, useSpring } from 'react-spring';

import Box from './Box';
import Flex from './Flex';
import FlexPanel from './FlexPanel';
import { Heading6 } from './Heading';
import Icon from './Icon';

const Accordion = ({
  children,
  openedSection,
  name,
  animated = true,
  onSectionToggle = () => {},
}) => (
  <FlexPanel>
    {React.Children.map(children, (child) =>
      React.cloneElement(child, {
        onSectionToggle: () =>
          onSectionToggle({ accordion: name, section: child.props.name }),
        open: openedSection === child.props.name,
        animated,
      })
    )}
  </FlexPanel>
);

const AccordionSection = ({
  children,
  open = false,
  disabled = false,
  name,
  animated,
  onSectionToggle,
  ...props
}) => {
  const childrenArray = Array.isArray(children) ? children : [children];
  return (
    <Flex
      flexDirection="column"
      name={name}
      disabled={disabled}
      open={open}
      borderBottom={2}
      borderColor="selects.tagBorder"
      {...props}
    >
      {childrenArray.map((child, index) =>
        child.type === AccordionHeading
          ? React.cloneElement(child, {
              key: child.key || index,
              onClick: onSectionToggle,
              open,
              disabled,
            })
          : React.cloneElement(child, {
              key: child.key || index,
              open,
              animated,
            })
      )}
    </Flex>
  );
};

/**
 *
 * @param {*} visibleWhen
 * @param {*} open
 * @return {bool}
 */
const isVisible = (visibleWhen, open) => {
  switch (visibleWhen) {
    case 'expanded':
      return open;
    case 'collapsed':
      return !open;
    case 'either':
    default:
      return true;
  }
};

/**
 * Handle icons rendering
 * @param {*} iconString
 * @param {*} open
 * @return {string}
 */
const getIcon = (iconString, open) => {
  const icons = iconString.split(/[, ]/);
  const idx = icons.length === 1 ? 0 : open ? 1 : 0;
  return icons[idx];
};

const AccordionHeading = ({
  iconPosition = 'right',
  icon = 'plus,minus',
  iconSize = 16,
  children,
  onClick,
  open,
  disabled,
  visibleWhen = 'either',
  ...props
}) =>
  isVisible(visibleWhen, open) ? (
    <Flex
      alignItems="center"
      hoverable={!disabled}
      pl={4}
      pr={3}
      justifyContent={iconPosition === 'right' && 'space-between'}
      onClick={!disabled ? onClick : null}
      {...props}
    >
      {iconPosition === 'left' && (
        <Icon
          size={iconSize}
          name={getIcon(icon, open)}
          color={disabled ? 'on.disabled' : undefined}
        />
      )}
      <Heading6
        pl={iconPosition === 'left' ? 3 : undefined}
        pr={iconPosition === 'right' ? 3 : undefined}
        py={3}
        uppercase
        color={disabled ? 'on.disabled' : undefined}
        fontWeight="normal"
      >
        {children}
      </Heading6>
      {iconPosition === 'right' && (
        <Icon
          size={iconSize}
          name={getIcon(icon, open)}
          color={disabled ? 'on.disabled' : undefined}
        />
      )}
    </Flex>
  ) : null;

const AnimatedPanel = styled(animated.div)`
  overflow-y: auto;
`;

const AccordionContent = ({ children, open, maxHeight = 600, animated }) => {
  let { height } = useSpring({
    from: { height: open ? 0 : maxHeight },
    height: open ? maxHeight : 0,
    config: config.fast,
    immediate: !animated,
  });
  return (
    <AnimatedPanel style={{ height }}>
      <Box p={4}>{children}</Box>
    </AnimatedPanel>
  );
};

Accordion.Section = AccordionSection;
Accordion.Heading = AccordionHeading;
Accordion.Content = AccordionContent;

export default Accordion;
