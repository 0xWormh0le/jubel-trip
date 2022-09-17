import WithRoot from './WithRoot';

export default class Tracker extends WithRoot {
  name;

  constructor(props = {}) {
    super(props);
    this.name = props.name || '_data';
  }

  get _q() {
    return this.root[this.name] || (this.root[this.name] = []);
  }

  push(func) {
    this._q.push(func);
  }
}
