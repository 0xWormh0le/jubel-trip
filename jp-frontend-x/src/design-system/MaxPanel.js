import system from '../utils/System';
import FlexPanel from './FlexPanel';

const MaxPanel = system('MaxPanel', {
  extend: FlexPanel,
  width: 1,
  flex: 1,
  height: '100%',
  minHeight: 0,
});
MaxPanel.displayName = 'MaxPanel';

export default MaxPanel;
