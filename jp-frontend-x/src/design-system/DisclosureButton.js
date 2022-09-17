import system from '../utils/System';
import IconButton from './IconButton';

const DisclosureButton = system('DisclosureButton', {
  extend: IconButton,
  size: 28,
  borderRadius: 'circle',
  borderWidth: 1,
  borderStyle: 'solid',
  color: 'on.border',
  bg: 'border',
});

export default DisclosureButton;
