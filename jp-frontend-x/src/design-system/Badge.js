import system from '../utils/System';
import Text from './Text';

const Badge = system('Badge', {
  extend: Text,
  letterSpacing: 'caps',
  borderRadius: 'circle',
  px: 1,
  opacity: 0.8,
  fontSize: 3,
  textAlign: 'center',
  textTransform: 'uppercase',
});

export default Badge;
