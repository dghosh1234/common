Option Explicit

Dim tbl, col
Dim targetTableCode
Dim found, affectedCount
Dim report
Dim runCheck

' Initialize
affectedCount = 0
found = False
runCheck = True
report = ""

' Prompt user for PHYSICAL table name (Code)
targetTableCode = InputBox("Enter the physical table name to check (case-insensitive):", "Target Table")

If targetTableCode = "" Then
    MsgBox "No table name entered. Script cancelled.", vbExclamation, "Cancelled"
    runCheck = False
End If

If runCheck Then
    report = "Columns in table '" & targetTableCode & "' that would be updated to VARCHAR2(n CHAR):" & vbCrLf

    ' Match on physical table name (Code)
    For Each tbl In ActiveModel.Tables
        If Not tbl.IsShortcut And UCase(tbl.Code) = UCase(targetTableCode) Then
            found = True
            For Each col In tbl.Columns
                If UCase(col.DataType) = "VARCHAR2" Then
                    If col.LengthSemantics <> "CHAR" Then
                        report = report & "- Column: " & col.Code & " (business name: " & col.Name & "), Length: " & col.Length & vbCrLf
                        affectedCount = affectedCount + 1
                    End If
                End If
            Next
        End If
    Next

    If Not found Then
        MsgBox "Table with code '" & targetTableCode & "' not found in the model.", vbExclamation, "Table Not Found"
    ElseIf affectedCount = 0 Then
        MsgBox "All VARCHAR2 columns in table '" & targetTableCode & "' already use CHAR semantics.", vbInformation, "Dry Run Complete"
    Else
        MsgBox report & vbCrLf & "Total: " & affectedCount & " column(s) would be updated.", vbInformation, "Dry Run Complete"
    End If
End If
