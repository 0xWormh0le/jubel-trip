import system from '../utils/System';
import Text from './Text';

const Button = system(
  'Button',
  {
    extend: Text,
    as: 'button',
    display: 'inline-block',
    py: 1,
    px: 2,
    borderRadius: 'sm',
    minWidth: 100,
    border: 1,
    hoverable: true,
  },
  (props) => ({
    appearance: 'none',
  })
);

export default Button;
