import system from '../utils/System';
import Box from './Box';

const Grid = system('Grid', {
  extend: Box,
  display: 'grid',
});

export default Grid;
