import { action, computed, observable, decorate } from 'mobx';

export default class Search {
  value = '';
  results = ['here are', 'some results', 'to view', 'as a test'];

  placeholderInterval;

  placeholders = [
    'Where do you want to go?',
    'What do you want to do there?',
    'When do you want to leave?',
  ];
  placeholderIndex = 0;

  constructor() {
    this.placeholderInterval = setInterval(
      () => this.cyclePlaceholders(),
      3500
    );
  }

  cyclePlaceholders() {
    this.placeholderIndex =
      (this.placeholderIndex + 1) % this.placeholders.length;
  }

  get placeholder() {
    return this.placeholders[this.placeholderIndex];
  }

  handleSubmit(event) {
    console.log('home search submit', event);
  }

  handleChange(event) {
    this.value = event;
    console.log('home search change', event);
  }
}

decorate(Search, {
  value: observable,
  results: observable,
  placeholderIndex: observable,
  cyclePlaceholders: action,
  placeholder: computed,
  handleSubmit: action,
  handleChange: action,
});
