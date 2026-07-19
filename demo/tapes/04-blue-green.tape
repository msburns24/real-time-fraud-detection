# Blue-green cutover under load — real run, real output.
Output demo/segments/segment4-blue-green.mp4
Output demo/segments/segment4-blue-green.gif

Set FontSize 15
Set Width 1500
Set Height 760
Set Theme "Catppuccin Mocha"
Set TypingSpeed 50ms
Set Padding 18
Set Framerate 12

Hide
Type "clear" Enter
Show

Type "# Zero-downtime cutover: load on :8080, switch fired mid-run" Enter
Sleep 1500ms
Type "bash scripts/bg_demo.sh 20 8" Enter
Sleep 27s
