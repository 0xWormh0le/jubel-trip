import system from '../utils/System';
import Flex from './Flex';

const TabButtonContainer = system(
  'TabButtonContainer',
  {
    extend: Flex,
  },
  (props) => ({
    flexWrap: 'wrap',
  })
);

export default TabButtonContainer;
