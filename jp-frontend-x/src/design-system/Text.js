import Box from './Box';
import system from '../utils/System';

const Text = system(
  'Text',
  {
    extend: Box,
  },
  ({ center, right }) => ({
    textAlign: (center || right) && (center ? 'center' : 'right'),
  }),
  ({ bold }) => ({
    fontWeight: bold && 'bold',
  }),
  ({ uppercase, lowercase, capitalize, nobr, truncate }) => ({
    textTransform: uppercase
      ? 'uppercase'
      : lowercase
      ? 'lowercase'
      : capitalize
      ? 'capitalize'
      : null,
    whiteSpace: truncate || nobr ? 'nowrap' : undefined,
    textOverflow: truncate ? 'ellipsis' : undefined,
    overflow: truncate ? 'hidden' : undefined,
  })
);

export default Text;
