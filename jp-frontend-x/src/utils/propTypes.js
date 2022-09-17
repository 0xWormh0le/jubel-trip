import PropTypes from 'prop-types';

export const numberString = PropTypes.oneOfType([
  PropTypes.number,
  PropTypes.string,
]);

export const numberStringOrArray = PropTypes.oneOfType([
  PropTypes.number,
  PropTypes.string,
  PropTypes.array,
]);

export const selectOptionType = PropTypes.shape({
  value: PropTypes.string,
  label: PropTypes.string,
});
