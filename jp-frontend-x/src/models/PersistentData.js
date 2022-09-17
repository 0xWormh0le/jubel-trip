import { action, computed, decorate, observable, toJS } from 'mobx';
import { isDate } from 'date-fns';
import WithRoot from './WithRoot';

export default class PersistentData extends WithRoot {
  name;
  saveInterval;
  saveTrigger = null;
  isLoaded = false;
  mutations;
  isValid = false;
  data = new Map();
  errors = new Map();

  constructor({ name = 'persistent', saveInterval = 1000 }) {
    super(arguments);
    this.name = name;
    this.saveInterval = saveInterval;
    this.mutations = new Set();
    this.isValid = false;
    this.errors.clear();
    this.data = this.getInitialData();
    this.data.forEach((value, key) => {
      this.errors.set(key, []);
    });
    this.load();
  }

  getInitialData() {
    return new Map();
  }

  get dataObject() {
    return toJS(this.data);
  }

  validate() {
    this.isValid = true;
  }

  setErrors(key, errors) {
    if (this.errors.has(key)) {
      this.errors.get(key).replace(errors);
    }
  }

  clearErrors(key) {
    if (this.errors.has(key)) {
      this.errors.get(key).clear();
    }
  }

  checkType(key, value) {
    let v = value;
    const logError = (t) => {
      console.error(
        `[${this.name}.${key}]: ignoring update value {${v}} because it is not a ${t}`
      );
    };
    if (this.data.has(key) && v !== null) {
      const d = this.data.get(key);
      if (d instanceof Date && !(v instanceof Date)) {
        if (['string', 'number'].includes(typeof v) && isDate(new Date(v))) {
          v = new Date(v);
        } else {
          logError('date');
          v = d;
        }
      } else if (typeof d === 'number' && typeof v !== 'number') {
        if (typeof v === 'string' && !isNaN(v)) {
          v = +v;
        } else {
          logError('number');
          v = d;
        }
      } else if (typeof d === 'boolean' && typeof v !== 'boolean') {
        if (typeof v === 'number') {
          v = Boolean(v);
        } else if (typeof v === 'string') {
          v = !['f', 'false', 'no', '0', ''].includes(v.toLowerCase());
        } else {
          logError('boolean');
          v = d;
        }
      }
    }
    return v;
  }

  update = (data) => {
    let changed = false;
    Object.keys(data).forEach((key) => {
      const value = data[key];
      // console.log('updating', {
      //   key,
      //   value,
      //   hasKey: this.data.has(key),
      //   data: this.dataObject,
      // });
      if (this.data.has(key)) {
        const v = this.checkType(key, value);
        // console.log('update', { v, value, dataValue: this.data.get(key) });
        if (v !== this.data.get(key) || key === 'searchText') {
          this.mutations.add(key);
          changed = true;
          this.data.set(key, v);
        }
      }
    });
    if (changed) {
      this.data.set('timestamp', new Date());
      this.validate();
      if (!this.saveTrigger) {
        // trigger initial save
        this.saveTrigger = setTimeout(() => this.save(), 0);
      }
    }
  };

  executeSearch = (key) => {
    if (key === 'Enter') {
      this.data.set('timestamp', new Date());
      this.validate();
      if (!this.saveTrigger) {
        // trigger initial save
        this.saveTrigger = setTimeout(() => this.save(), 0);
      }
    }
  };

  get dirty() {
    return this.mutations.size > 0;
  }

  fromSavedObject(obj) {
    return obj;
  }

  load(maybeJson) {
    const fromURL = !!maybeJson;
    const json = maybeJson || localStorage.getItem(this.name);
    if (json) {
      try {
        const obj = this.fromSavedObject(fromURL ? json : JSON.parse(json));
        Object.keys(obj).forEach((k) => {
          this.data.set(k, obj[k]);
          // console.log(k, obj[k]);
        });
        this.mutations.clear();
        this.validate();
        this.isLoaded = true;
      } catch (e) {
        console.error(
          '*** Failed to load preferences from local storage: ',
          e.message
        );
      }
    }
  }

  save() {
    // console.log('save: mutations size = ', this.mutations.size);
    if (this.saveTrigger) clearTimeout(this.saveTrigger);
    if (this.mutations.size > 0) {
      const obj = this.dataObject;
      // console.log('saving', obj, JSON.stringify(obj));
      localStorage.setItem(this.name, JSON.stringify(obj));
      this.mutations.clear();
    }
    // console.log('setting save trigger', this.saveInterval);
    this.saveTrigger = setTimeout(() => this.save(), this.saveInterval);
  }
}

decorate(PersistentData, {
  isLoaded: observable,
  isValid: observable,
  data: observable,
  errors: observable,
  dataObject: computed,
  validate: action,
  setErrors: action,
  clearErrors: action,
  update: action,
  dirty: computed,
  load: action,
});
