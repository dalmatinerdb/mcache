#include "stdio.h"
#include <stdint.h>
uint8_t meq(uint8_t *left, uint8_t *right, size_t count) {
  for (;count > 8; count -= 8, left += 8, right += 8) {
    uint64_t vl = *((uint64_t *) left);
    uint64_t vr = *((uint64_t *) right);
    if (vl != vr) {
      return 0;
    };
  }
  for (; count--; left++, right++) {
    if (*left != *right) {
      return 0;
    }
  }

  return 1;
}
