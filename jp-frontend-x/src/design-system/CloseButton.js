import system from '../utils/System';
import IconButton from './IconButton';

const CloseButton = system('CloseButton', {
  extend: IconButton,
  name: 'close',
  position: 'absolute',
  right: '0.5rem',
  top: '0.5rem',
  color: 'misc.muted',
});

export default CloseButton;
