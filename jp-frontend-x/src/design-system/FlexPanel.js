import system from '../utils/System';
import Flex from './Flex';

const FlexPanel = system(
  'FlexPanel',
  {
    extend: Flex,
    flexDirection: 'column',
  },
  (props) => ({
    justifyContent: props.centered && 'center',
    alignItems: props.centered && 'center',
    minHeight: 0,
  })
);

export default FlexPanel;
