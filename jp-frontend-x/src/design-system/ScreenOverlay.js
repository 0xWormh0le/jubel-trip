import system from '../utils/System';
import Absolute from './Absolute';

const ScreenOverlay = system('ScreenOverlay', {
  extend: Absolute,
  position: 'absolute',
  top: 0,
  bottom: 0,
  left: 0,
  right: 0,
  zIndex: 1000,
  bg: 'rgba(1,1,1,.5)',
});

export default ScreenOverlay;
