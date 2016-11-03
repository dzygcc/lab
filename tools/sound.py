# -*- coding: utf-8 -*-

try:
    import winsound

    def make_sound():
        winsound.Beep(3072, 60)
        winsound.Beep(2048, 100)
except ImportError:
    def make_sound():
        pass

if __name__ == "__main__":
    print("ab")
    make_sound()
    make_sound()
    make_sound()
    print("cd")