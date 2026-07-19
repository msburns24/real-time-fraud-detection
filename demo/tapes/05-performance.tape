Output demo/segments/segment5-performance.mp4
Output demo/segments/segment5-performance.gif

Set FontSize 15
Set Width 1500
Set Height 560
Set Theme "Catppuccin Mocha"
Set TypingSpeed 50ms
Set Padding 18
Set Framerate 12

Hide
Type "clear" Enter
Show

# --out sends the JSON to a scratch path. Recording this segment must not
# overwrite results.json, which is the committed artifact the report cites.
Type "python tests/test_performance.py --n 5000 --out /tmp/demo_results.json" Enter
Sleep 12s
