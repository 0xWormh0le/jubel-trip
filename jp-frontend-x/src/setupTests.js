import { configure } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
import * as emotion from 'emotion';
import { createSerializer } from 'jest-emotion';

expect.addSnapshotSerializer(createSerializer(emotion));
configure({ adapter: new Adapter() });

if (global.document) {
  document.createRange = () => ({
    setStart: () => {},
    setEnd: () => {},
    commonAncestorContainer: {
      nodeName: 'BODY',
      ownerDocument: document,
    },
  });
}
