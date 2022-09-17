import system from '../utils/System';
import Box from './Box';

const Image = system('Image', {
  extend: Box,
  as: 'img',
  maxWidth: '100%',
  height: 'auto',
  display: 'inline-block',
});

export default Image;
