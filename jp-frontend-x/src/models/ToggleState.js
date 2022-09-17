import { action, observable, computed, decorate } from 'mobx';

export default class ToggleState {
  _open = false;
  name = 'toggler';

  constructor(props = {}) {
    if (typeof props === 'string') this.name = props;
    else {
      if (props.name) this.name = props.name;
      if (props.open) this._open = props.open;
    }
  }

  get isOpen() {
    return this._open;
  }

  get isClosed() {
    return !this._open;
  }

  open = () => {
    this._open = true;
  };

  close = () => {
    this._open = false;
  };

  toggle = (value) => {
    if (typeof value === 'boolean') this._open = value;
    else this._open = !this._open;
  };

  toString() {
    return `${this.name} toggle state: ${this.isOpen ? 'open' : 'closed'}`;
  }
}

decorate(ToggleState, {
  _open: observable,
  isOpen: computed,
  isClosed: computed,
  open: action,
  close: action,
  toggle: action,
});
