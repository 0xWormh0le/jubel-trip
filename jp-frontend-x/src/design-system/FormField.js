import PropTypes from 'prop-types';
import React from 'react';
import Box from './Box';
import Flex from './Flex';
import Icon from './Icon';
import Label from './Label';
import Input from './Input';
import Select from './Select';
import { keyframes } from '@emotion/core';

const fadeIn = keyframes`
  from {
    opacity: 0;
  }

  to {
    opacity: 1;
  }
`;
const labelStyles = {
  animation: fadeIn + ' 0.3s',
};

const noop = () => {};

const isFormElement = () => {};

export class FormField extends React.Component {
  static defaultProps = {
    // for backwards-compatibility
    onChange: noop,
    homeSearch: false,
    boxShadow: false,
  };
  static formElements = [Input, Select];

  isFormElement = (element) => FormField.formElements.includes(element);
  // for backwards-compatibility
  handleChange = (onChange) => (e) => {
    this.props.onChange(e);
    if (typeof onChange !== 'function') return;
    onChange(e);
  };
  hasValue = () => {
    const { children } = this.props;
    return React.Children.toArray(children).reduce(
      (a, child) =>
        a || (child && isFormElement(child.type) && child.props.value),
      false
    );
  };
  render() {
    const {
      label,
      icon,
      children,
      onChange,
      homeSearch,
      boxShadow,
      ...props
    } = this.props;
    let FieldChild;
    let position = -1;
    let LabelChild;
    let BeforeIcon;
    let AfterIcon;
    let fieldId;
    let fieldPlaceholder;
    React.Children.forEach(children, (child, index) => {
      if (!child) return;
      const { type, props } = child;
      switch (type) {
        case Label:
          LabelChild = child;
          break;
        case Input:
        case Select:
          position = index;
          FieldChild = child;
          fieldId = props.id;
          // For aria-label when Label child is not rendered
          fieldPlaceholder = props.placeholder;
          break;
        case Icon:
          if (position < 0) {
            BeforeIcon = child;
          } else {
            AfterIcon = child;
          }
          break;
        default:
          break;
      }
    });
    const showLabel =
      LabelChild && LabelChild.props.hidden
        ? false
        : this.props.alwaysShowLabel || (LabelChild && this.hasValue());
    return (
      <Box>
        <Flex alignItems="center" width={1} mt={0}>
          {showLabel &&
            React.cloneElement(LabelChild, {
              pl: 1,
              style: labelStyles,
              htmlFor: fieldId,
            })}
          <Flex width="100%" alignItems="center">
            {BeforeIcon && (
              <Flex mr={-1} mt={showLabel ? 0 : 1} px={3} alignItems="centar">
                {BeforeIcon}
              </Flex>
            )}
            {React.cloneElement(FieldChild, {
              'aria-label':
                !showLabel && fieldPlaceholder ? fieldPlaceholder : null,
              boxShadow: boxShadow ? 1 : null,
              border: 0,
              transition: 'padding-top 0.1s, padding-bottom 0.1s',
              width: 1,
              ml: showLabel && BeforeIcon ? -5 : 0,
              mt: showLabel && -2,
              pl: BeforeIcon ? (showLabel ? 5 : 0) : 3,
              pr: AfterIcon && 2,
              pt: showLabel ? 3 : 2,
              pb: 2,
              // ref: (elem) => {
              //   this.fieldRef = elem;
              // },
              // for backwards compatibility
              onChange: this.handleChange(FieldChild.props.onChange),
              ...props,
            })}
          </Flex>
          {AfterIcon && (
            <Box
              alignItems="center"
              ml={-1}
              mt={showLabel ? -2 : 0}
              bg={AfterIcon.props.bg}
              style={{
                height: homeSearch ? '4rem' : null,
                width: homeSearch ? '5rem' : 0,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                borderBottomRightRadius: 5,
                borderTopRightRadius: 5,
              }}
            >
              {AfterIcon}
            </Box>
          )}
        </Flex>
      </Box>
    );
  }
}

FormField.propTypes = {
  alwaysShowLabel: PropTypes.bool,
  homeSearch: PropTypes.bool,
  children: PropTypes.any,
};

export default FormField;
