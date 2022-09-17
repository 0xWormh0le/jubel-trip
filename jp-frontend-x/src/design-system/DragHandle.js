import system from '../utils/System';
import IconButton from './IconButton';

const DragHandle = system('DragHandle', {
  extend: IconButton,
  position: 'absolute',
  name: 'drag-vertical',
  py: 3,
  px: 0,
  top: '50%',
  transform: 'translate(0,-50%)',
  bg: 'secondary',
  right: 0,
  cursor: 'col-resize',
});

export default DragHandle;
