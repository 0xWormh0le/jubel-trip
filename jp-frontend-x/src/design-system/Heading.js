import system from '../utils/System';
import Text from './Text';

export const Heading1 = system('Heading1', {
  extend: Text,
  as: 'h1',
  className: 'heading',
});

export const Heading2 = system('Heading2', {
  extend: Text,
  as: 'h2',
  className: 'heading',
});

export const Heading3 = system('Heading3', {
  extend: Text,
  as: 'h3',
  className: 'heading',
});

export const Heading4 = system('Heading4', {
  extend: Text,
  as: 'h4',
  className: 'heading',
});

export const Heading5 = system('Heading5', {
  extend: Text,
  as: 'h5',
  className: 'heading',
});

export const Heading6 = system('Heading6', {
  extend: Text,
  as: 'h6',
  className: 'heading',
});

export default Heading2;
