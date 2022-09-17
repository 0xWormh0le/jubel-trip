import { variant } from 'styled-system';
import PropTypes from 'prop-types';

const dynamicVariant = (variantProps) => {
  const { prop = 'variant' } = variantProps;
  const defaultVariant = variant(variantProps);

  const fn = (props) => {
    const { [prop]: variantProp } = props;

    if (!Array.isArray(variantProp)) {
      return defaultVariant;
    }
  };

  fn.propTypes = { [prop]: PropTypes.func };

  return fn;
};

export default dynamicVariant;
