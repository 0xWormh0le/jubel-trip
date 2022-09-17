import system from '../utils/System';
import Text from './Text';

const nowrap = (props) =>
  props.nowrap
    ? {
        whiteSpace: 'nowrap',
      }
    : null;

const accessiblyHide = (props) =>
  props.hidden
    ? {
        position: 'absolute',
        left: '-10000px',
        top: 'auto',
        width: '1px',
        height: '1px',
        overflow: 'hidden',
      }
    : null;

const Label = system(
  'Label',
  {
    extend: Text,
    as: 'label',
    display: 'inline-block',
    fontWeight: 'bold',
    nowrap: false,
    hidden: false,
  },
  nowrap,
  accessiblyHide
);

export default Label;
