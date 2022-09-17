import system from '../utils/System';
import Box from './Box';

const Flex = system(
  'Flex',
  {
    extend: Box,
    display: 'flex',
  },
  ({ centered }) => ({
    justifyContent: centered && 'center',
    alignItems: centered && 'center',
  })
);

export default Flex;
