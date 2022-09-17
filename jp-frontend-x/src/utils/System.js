/* eslint-disable react/forbid-foreign-prop-types */
import styled from '@emotion/styled';
import isPropValid from '@emotion/is-prop-valid';
import { omit, get, keys, uniq } from 'lodash';
import * as ssexports from 'styled-system';

/**
 * This file is based closely on @rebass/components...just reproducing it here
 * to simplify dependency management.
 */
const styles = omit(ssexports, [
  'defaultBreakpoints',
  'cloneFunction',
  'get',
  'themeGet',
  'is',
  'isObject',
  'num',
  'px',
  'createMediaQuery',
  'style',
  'compose',
  'mapProps',
  'variant',
]);

const styleKeys = Object.keys(styles).filter(
  (key) => typeof styles[key] === 'function'
);

const propNames = uniq(
  styleKeys.reduce((a, key) => [...a, ...keys(styles[key].propTypes)], [])
);

const extensionProps = ['is', 'as', 'tag', 'extend'];
const otherProps = [
  'animated',
  'primary',
  'secondary',
  'success',
  'info',
  'warning',
  'danger',
  'error',
  'inline',
  'lowercase',
  'uppercase',
  'capitalize',
  'nowrap',
  'gray',
  'hidden',
  'hoverable',
  'truncate',
  'plain',
  'centered',
];
const _blacklist = ['css', ...extensionProps, ...propNames, ...otherProps]; // .filter(p => !['width', 'height'].includes(p));

const getPropTypes = (styleFuncs) =>
  styleFuncs
    .filter(
      (fn) => typeof fn === 'function' && typeof fn.propTypes === 'object'
    )
    .reduce(
      (a, fn) => ({
        ...a,
        ...fn.propTypes,
      }),
      {}
    );

const css = (props) => props.css;

const shouldForwardProp = (prop) => {
  const isValid = isPropValid(prop);
  const isBlacklisted = _blacklist.includes(prop);
  return isValid && !isBlacklisted;
};

const system = (name, props = {}, ...keysOrStyles) => {
  const requestedStyles = keysOrStyles.map(
    (keyOrStyle) => styles[keyOrStyle] || keyOrStyle
  );
  const propTypes = getPropTypes(requestedStyles);
  const baseProps = get(props, 'extend.defaultProps', {});
  const localProps = omit(props, extensionProps);
  const { extend } = props;
  const asTag = props.as || props.tag || props.is;
  const Base = extend || asTag || 'div';
  if (extend && asTag) {
    localProps.as = asTag;
  }
  const opts = typeof Base === 'string' && { shouldForwardProp };
  const comp = styled(Base, opts)(...requestedStyles, css);
  comp.defaultProps = { ...baseProps, ...localProps };
  comp.propTypes = propTypes;
  comp.systemComponent = true;
  comp.displayName = name;
  return comp;
};

export default system;
