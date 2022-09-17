import { action, observable, computed, decorate } from 'mobx';
import ToggleState from './ToggleState';

export default class AccordionState {
  name = 'accordion';
  multi = false;
  //@observable
  sections = new Map();

  constructor(props = {}) {
    if (props.name) this.name = props.name;
    if (props.multi) this.multi = props.multi;
    if (props.sections) this.setSections(props.sections);
  }

  //@computed
  get isSingle() {
    return !this.multi;
  }

  //@computed
  get isMulti() {
    return this.multi;
  }

  //@computed
  get openedSections() {
    return this.sections.entries
      .filter(([s, t]) => t.isOpen)
      .map(([s, t]) => s);
  }

  //@action
  setSections(sections = {}) {
    this.sections.clear();
    let openOK = true;
    sections.forEach((_open, section) => {
      const name = `${this.name}-${section}`;
      const open = (this.isMulti || openOK) && _open;
      openOK = this.isMulti || !open;
      this.sections.set(section, new ToggleState({ name, open }));
    });
  }

  //@action
  onSectionToggle = (section, toggled) => {
    if (this.sections.has(section)) {
      const toggle = this.sections.get(section);
      toggle.toggle(toggled);
    }
  };
}

decorate(AccordionState, {
  sections: observable,
  isMulti: computed,
  isSingle: computed,
  openSections: computed,
  setSections: action,
  onSectionToggle: action,
});
