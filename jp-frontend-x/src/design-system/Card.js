import system from '../utils/System';
import Flex from './Box';

const Card = system('Card', {
  extend: Flex,
  borderRadius: 'sm',
  boxShadow: 4,
  className: 'surface',
  bg: 'surface',
});

export default Card;
