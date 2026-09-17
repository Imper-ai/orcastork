import { describe, expect, it } from 'vitest';
import {
  CompletionTailTimeoutError,
  DuplicateRegistrationError,
  OrchestrationError,
  StaleEpochError,
} from '../src/orcastork/exceptions.js';

describe('the exception hierarchy', () => {
  it('roots every framework error at OrchestrationError', () => {
    const error = new StaleEpochError('epoch 2 is behind 3');

    expect(error).toBeInstanceOf(StaleEpochError);
    expect(error).toBeInstanceOf(OrchestrationError);
    expect(error).toBeInstanceOf(Error);
  });

  it('names each error after its own class, so a caught error identifies itself', () => {
    expect(new OrchestrationError('x').name).toBe('OrchestrationError');
    expect(new DuplicateRegistrationError('x').name).toBe('DuplicateRegistrationError');
    expect(new CompletionTailTimeoutError('x').name).toBe('CompletionTailTimeoutError');
  });

  it('keeps the message and the cause it was given', () => {
    const cause = new Error('underlying');
    const error = new CompletionTailTimeoutError('flush outlived its timeout', { cause });

    expect(error.message).toBe('flush outlived its timeout');
    expect(error.cause).toBe(cause);
    expect(String(error)).toBe('CompletionTailTimeoutError: flush outlived its timeout');
  });
});
