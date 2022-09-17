import { get } from 'lodash';

export default class WithRoot {
  root;

  constructor(props = {}) {
    this.root = props.root || global || window || {};
  }

  _root(path, defaultValue) {
    return get(this.root, path, defaultValue);
  }
}
