import system from '../utils/System';
import IconButton from './IconButton';

const ReviewButton = system('ReviewButton', {
  extend: IconButton,
  size: 16,
  color: 'misc.muted',
  name: 'review-icon',
  labelMargin: 2,
});

export default ReviewButton;
