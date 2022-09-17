import system from '../utils/System';
import Box from './Box';
import theme from './theme';

const Container = system('Container', {
  extend: Box,
  maxWidth: theme.maxWidths,
  mx: 'auto',
});

export default Container;
